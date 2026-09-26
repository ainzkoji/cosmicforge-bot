"""FX price-scale verification and full-history cross-rate QA (Section 12.2-12.4, 12.17).

Why this exists
---------------
Dukascopy ``.bi5`` files store prices as integers x a per-instrument point.
The point is NOT constant over time: the EURCNH and EURZAR files up to and
including August 2024 carry six decimals (EURCNH raw close ~7,861,043 =
7.861043) and from September 2024 five (~786,027 = 7.86027), while their USD
legs keep five decimals throughout (raw files verified 2026-09-26). A decoder
that applies one fixed point per pair (``fx_reference.point_for``) silently
stored those months 10x too high; a validator that only looked at the LATEST
bar could not see it.

So the scale of every provider file is VERIFIED, never assumed:

* ``infer_point``: the candidate power-of-ten point whose decoded median lies
  within ``tolerance`` of an independent expected level (the triangular level
  implied by the pair's two USD legs over the same period, else the pair's
  own adjacent verified period). Exactly one candidate must match; zero or
  several -> ``None`` (the period is quarantined, never guessed).
* ``scale_breaks``: within-pair per-period levels whose log10 distance from
  the pair's overall median is >= ``break_log10`` (~8x) -- a power-of-ten
  style discontinuity no FX market produces inside the dataset window.
* ``cross_rate_history``: every triangular relation X/Y ~ (X/USD)/(Y/USD) over
  ALL aligned bars (not the latest one): median / p95 / p99 / max relative
  error, the timestamp of the max, per-period medians, pass/fail.

QA never rewrites a price. Repairs go through the controlled re-ingest in
``scripts/acquire_fx_reference_dataset.py repair-scale`` with lineage in
``fx_reference_repairs``.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

SCALE_QA_VERSION = "fx-scale-qa-v1"
POINT_CANDIDATES = (1e2, 1e3, 1e4, 1e5, 1e6, 1e7)
#: a verified scale puts the decoded median within 25% of the independent level; the candidates are 10x apart,
#: so at most one can match and genuine monthly FX moves (a few %) never flip the choice
DEFAULT_TOLERANCE = 0.25
#: ~8x: a within-pair period level this far from the pair's overall median is a scale break, not a market move
DEFAULT_BREAK_LOG10 = 0.9
#: cross-rate relation thresholds (QA evidence only)
PERIOD_MEDIAN_MAX = 0.005     # a period whose median triangular error exceeds 0.5% fails
BAR_MAX = 0.5                 # any single aligned bar off by >= 50% is a scale-style failure


def median(values: Sequence[float]) -> Optional[float]:
    return statistics.median(values) if values else None


def infer_point(raw_values: Sequence[int], expected_level: Optional[float], *,
                candidates: Sequence[float] = POINT_CANDIDATES,
                tolerance: float = DEFAULT_TOLERANCE) -> Tuple[Optional[float], Dict[str, Any]]:
    """(point, evidence). ``raw_values``: the integer prices of one provider file."""
    raw = [v for v in raw_values if v]
    if not raw:
        return None, {"reason": "NO_RAW_VALUES"}
    if expected_level is None or expected_level <= 0:
        return None, {"reason": "NO_INDEPENDENT_LEVEL"}
    m = median(raw)
    fits = [p for p in candidates if abs(m / p - expected_level) / expected_level <= tolerance]
    ev = {"raw_median": m, "expected_level": expected_level, "matching_points": fits, "tolerance": tolerance}
    if len(fits) != 1:
        return None, {**ev, "reason": "AMBIGUOUS_SCALE" if fits else "NO_SCALE_MATCHES_EXPECTED_LEVEL"}
    return fits[0], {**ev, "reason": "VERIFIED"}


def scale_breaks(period_levels: Mapping[str, Optional[float]], *,
                 break_log10: float = DEFAULT_BREAK_LOG10) -> Dict[str, Dict[str, Any]]:
    """{period: {level, ratio_to_pair_median, factor}} for periods that break the pair's scale."""
    levels = {p: v for p, v in period_levels.items() if v and v > 0}
    if len(levels) < 3:
        return {}
    ref = median(list(levels.values()))
    out = {}
    for p, v in sorted(levels.items()):
        d = math.log10(v / ref)
        if abs(d) >= break_log10:
            out[p] = {"level": v, "pair_median_level": ref, "ratio": v / ref, "factor": 10 ** round(d)}
    return out


def usd_legs(pair: str, available: Iterable[str]) -> Optional[Tuple[Tuple[str, bool], Tuple[str, bool]]]:
    """For X/Y without USD: ((leg_for_X, direct), (leg_for_Y, direct)) where direct means CCYUSD."""
    avail = set(available)
    a, b = pair[:3], pair[3:]
    if "USD" in (a, b):
        return None

    def leg(ccy: str) -> Optional[Tuple[str, bool]]:
        if f"{ccy}USD" in avail:
            return f"{ccy}USD", True
        if f"USD{ccy}" in avail:
            return f"USD{ccy}", False
        return None

    la, lb = leg(a), leg(b)
    return (la, lb) if la and lb else None


def implied_cross(la_value: float, la_direct: bool, lb_value: float, lb_direct: bool) -> float:
    """X/Y from the USD legs: (X in USD) / (Y in USD)."""
    x = la_value if la_direct else 1.0 / la_value
    y = lb_value if lb_direct else 1.0 / lb_value
    return x / y


def _pct(sorted_values: Sequence[float], q: float) -> Optional[float]:
    if not sorted_values:
        return None
    i = min(len(sorted_values) - 1, max(0, int(math.ceil(q * len(sorted_values))) - 1))
    return sorted_values[i]


def _month(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y-%m")


def triangular_level(conn: Any, *, provider: str, pair: str, timeframe: str, start_ms: int, end_ms: int,
                     available: Iterable[str]) -> Optional[float]:
    """Median implied X/Y over [start, end) from the stored USD legs (an independent level)."""
    legs = usd_legs(pair, available)
    if legs is None:
        return None
    (la, da), (lb, db) = legs
    rows = conn.execute(
        "SELECT a.mid_close, b.mid_close FROM fx_reference_quotes a JOIN fx_reference_quotes b "
        "ON b.provider=a.provider AND b.timeframe=a.timeframe AND b.open_time=a.open_time "
        "WHERE a.provider=? AND a.timeframe=? AND a.pair=? AND b.pair=? AND a.open_time>=? AND a.open_time<? "
        "AND a.mid_close IS NOT NULL AND b.mid_close IS NOT NULL",
        (provider, timeframe, la, lb, start_ms, end_ms)).fetchall()
    return median([implied_cross(x, da, y, db) for x, y in rows]) if rows else None


@dataclass
class RelationQA:
    pair: str
    legs: Tuple[str, str]
    aligned: int
    median_rel: Optional[float]
    p95_rel: Optional[float]
    p99_rel: Optional[float]
    max_rel: Optional[float]
    max_at_ms: Optional[int]
    period_medians: Dict[str, float]
    failing_periods: List[str]

    @property
    def passed(self) -> bool:
        return self.aligned > 0 and not self.failing_periods and (self.max_rel or 0.0) < BAR_MAX

    def to_dict(self) -> Dict[str, Any]:
        return {"pair": self.pair, "legs": list(self.legs), "aligned_bars": self.aligned,
                "median_rel_error": self.median_rel, "p95_rel_error": self.p95_rel, "p99_rel_error": self.p99_rel,
                "max_rel_error": self.max_rel, "max_rel_error_at_ms": self.max_at_ms,
                "max_rel_error_at": (datetime.fromtimestamp(self.max_at_ms / 1000, timezone.utc).isoformat()
                                     if self.max_at_ms is not None else None),
                "failing_periods": self.failing_periods, "passed": self.passed,
                "period_median_rel_error": dict(sorted(self.period_medians.items()))}


def cross_rate_history(conn: Any, *, provider: str, timeframe: str, pairs: Sequence[str]) -> List[RelationQA]:
    """Every triangular relation over the FULL overlapping history (streamed per relation)."""
    out = []
    for pair in sorted(pairs):
        legs = usd_legs(pair, pairs)
        if legs is None:
            continue
        (la, da), (lb, db) = legs
        cur = conn.execute(
            "SELECT x.open_time, x.mid_close, a.mid_close, b.mid_close FROM fx_reference_quotes x "
            "JOIN fx_reference_quotes a ON a.provider=x.provider AND a.timeframe=x.timeframe AND "
            "a.open_time=x.open_time AND a.pair=? "
            "JOIN fx_reference_quotes b ON b.provider=x.provider AND b.timeframe=x.timeframe AND "
            "b.open_time=x.open_time AND b.pair=? "
            "WHERE x.provider=? AND x.timeframe=? AND x.pair=? AND x.mid_close IS NOT NULL AND "
            "a.mid_close IS NOT NULL AND b.mid_close IS NOT NULL ORDER BY x.open_time",
            (la, lb, provider, timeframe, pair))
        errs: List[float] = []
        per: Dict[str, List[float]] = {}
        worst, worst_t = -1.0, None
        for t, x, av, bv in cur:
            implied = implied_cross(av, da, bv, db)
            e = abs(x - implied) / implied
            errs.append(e)
            per.setdefault(_month(t), []).append(e)
            if e > worst:
                worst, worst_t = e, t
        s = sorted(errs)
        pm = {p: statistics.median(v) for p, v in per.items()}
        failing = sorted(p for p, v in pm.items() if v > PERIOD_MEDIAN_MAX or max(per[p]) >= BAR_MAX)
        out.append(RelationQA(pair, (la, lb), len(s), median(s), _pct(s, 0.95), _pct(s, 0.99),
                              s[-1] if s else None, worst_t, pm, failing))
    return out


def period_levels(conn: Any, *, provider: str, pair: str, timeframe: str) -> Dict[str, float]:
    rows = conn.execute("SELECT open_time, mid_close FROM fx_reference_quotes WHERE provider=? AND pair=? AND "
                        "timeframe=? AND mid_close IS NOT NULL ORDER BY open_time", (provider, pair, timeframe))
    per: Dict[str, List[float]] = {}
    for t, v in rows:
        per.setdefault(_month(t), []).append(v)
    return {p: statistics.median(v) for p, v in per.items()}


def scale_report(conn: Any, *, provider: str, timeframe: str, pairs: Sequence[str]) -> Dict[str, Any]:
    """Per-pair scale status from two independent checks (within-pair breaks + cross-rate periods)."""
    rel = {r.pair: r for r in cross_rate_history(conn, provider=provider, timeframe=timeframe, pairs=pairs)}
    out: Dict[str, Any] = {"version": SCALE_QA_VERSION, "provider": provider, "timeframe": timeframe, "pairs": {},
                           "relations": {p: r.to_dict() for p, r in sorted(rel.items())}}
    for pair in sorted(pairs):
        breaks = scale_breaks(period_levels(conn, provider=provider, pair=pair, timeframe=timeframe))
        r = rel.get(pair)
        cross_fail = r.failing_periods if r else []
        status = "SCALE_BREAK" if (breaks or (r is not None and not r.passed)) else (
            "VERIFIED_CROSS_RATE" if r is not None else "VERIFIED_CONTINUITY")
        out["pairs"][pair] = {"status": status, "scale_break_periods": breaks, "cross_rate_failing_periods": cross_fail}
    out["failing_pairs"] = sorted(p for p, v in out["pairs"].items() if v["status"] == "SCALE_BREAK")
    return out


__all__ = ["BAR_MAX", "DEFAULT_BREAK_LOG10", "DEFAULT_TOLERANCE", "PERIOD_MEDIAN_MAX", "POINT_CANDIDATES",
           "RelationQA", "SCALE_QA_VERSION", "cross_rate_history", "implied_cross", "infer_point", "period_levels",
           "scale_breaks", "scale_report", "triangular_level", "usd_legs"]
