"""Certification statistics (Sections 22.14-22.23). Pure, deterministic.

* expectancy is AFTER cost (the caller passes net R); win rate is reported
  but never decides anything on its own.
* intervals use a CIRCULAR BLOCK bootstrap in chronological order (block
  length ``ceil(n ** (1/3))``) because trade outcomes are serially dependent;
  the seed is part of the certification policy identity.
* too-small samples return ``INSUFFICIENT_EVIDENCE`` -- never a zero.
"""
from __future__ import annotations

import itertools
import math
import random
from statistics import NormalDist, median
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
_N = NormalDist()


def block_length(n: int) -> int:
    return max(1, math.ceil(n ** (1.0 / 3.0))) if n > 0 else 1


def _bootstrap_means(values: Sequence[float], *, reps: int, seed: int, block: int) -> List[float]:
    n = len(values)
    rng = random.Random(seed)
    means = []
    for _ in range(reps):
        total, taken = 0.0, 0
        while taken < n:
            start = rng.randrange(n)
            for j in range(min(block, n - taken)):
                total += values[(start + j) % n]
            taken += min(block, n - taken)
        means.append(total / n)
    return means


def _quantile(sorted_values: Sequence[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    pos = q * (len(sorted_values) - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (pos - lo)


def expectancy_summary(values: Sequence[float], *, reps: int, confidence: float, seed: int, min_n: int = 2,
                       outcomes: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    vals = [float(v) for v in values]
    n = len(vals)
    out: Dict[str, Any] = {"n": n, "method": "CIRCULAR_BLOCK_BOOTSTRAP", "block_rule": "ceil(n**(1/3))",
                           "seed": seed, "repetitions": reps, "confidence": confidence}
    if outcomes is not None:
        counts = {k: sum(1 for o in outcomes if o == k) for k in ("TARGET_BEFORE_STOP", "STOP_BEFORE_TARGET", "TIMEOUT")}
        out["outcome_counts"] = counts
    if n < max(2, min_n):
        out.update(status=INSUFFICIENT, mean_R=(sum(vals) / n if n else None), median_R=(median(vals) if n else None))
        return out
    mean = sum(vals) / n
    sd = math.sqrt(sum((v - mean) ** 2 for v in vals) / (n - 1))
    block = block_length(n)
    means = sorted(_bootstrap_means(vals, reps=reps, seed=seed, block=block))
    alpha = (1.0 - confidence) / 2.0
    wins = sum(1 for v in vals if v > 0)
    sv = sorted(vals)
    out.update(
        status="OK", mean_R=mean, median_R=median(vals), std_R=sd, block_length=block,
        ci_low=_quantile(means, alpha), ci_high=_quantile(means, 1.0 - alpha),
        p_expectancy_positive=sum(1 for m in means if m > 0) / len(means),
        win_count=wins, loss_count=sum(1 for v in vals if v < 0), win_rate=wins / n,
        distribution={"p05": _quantile(sv, 0.05), "p25": _quantile(sv, 0.25), "p50": _quantile(sv, 0.50),
                      "p75": _quantile(sv, 0.75), "p95": _quantile(sv, 0.95), "min": sv[0], "max": sv[-1]},
    )
    return out


def drawdown_and_tail(values: Sequence[float], times: Optional[Sequence[int]] = None, *, es_alpha: float = 0.05,
                      min_tail_samples: int = 20) -> Dict[str, Any]:
    """Cumulative-R drawdown over the chronological trade sequence. Evidence,
    never a new hard-risk rule."""
    vals = [float(v) for v in values]
    n = len(vals)
    if n == 0:
        return {"n": 0, "status": INSUFFICIENT}
    peak = equity = 0.0
    max_dd = 0.0
    dd_start = dur = max_dur = 0
    dd_ms = max_dd_ms = 0
    streak = max_streak = 0
    for i, v in enumerate(vals):
        equity += v
        if equity >= peak:
            peak, dur, dd_start = equity, 0, i
        else:
            dur = i - dd_start
            max_dur = max(max_dur, dur)
            if times is not None:
                dd_ms = int(times[i]) - int(times[dd_start])
                max_dd_ms = max(max_dd_ms, dd_ms)
        max_dd = max(max_dd, peak - equity)
        streak = streak + 1 if v < 0 else 0
        max_streak = max(max_streak, streak)
    sv = sorted(vals)
    out = {"n": n, "status": "OK", "max_drawdown_R": max_dd, "max_drawdown_duration_trades": max_dur,
           "max_drawdown_duration_ms": max_dd_ms if times is not None else None, "worst_trade_R": sv[0],
           "best_trade_R": sv[-1], "max_loss_streak": max_streak, "final_cumulative_R": equity}
    if n >= min_tail_samples:
        k = max(1, int(math.floor(n * es_alpha)))
        out["expected_shortfall_R"] = sum(sv[:k]) / k
        out["expected_shortfall_alpha"] = es_alpha
    else:
        out["expected_shortfall_R"] = None
        out["expected_shortfall_status"] = INSUFFICIENT
    return out


def calibration_summary(pairs: Sequence[Tuple[float, int]], *, bins: int = 10, min_n: int = 1) -> Dict[str, Any]:
    """Probabilities judged AS probabilities: reliability buckets, Brier,
    log score (a proper scoring rule), ECE and Brier skill vs the base rate."""
    n = len(pairs)
    if n < max(1, min_n):
        return {"n": n, "status": INSUFFICIENT}
    eps = 1e-6
    brier = sum((p - y) ** 2 for p, y in pairs) / n
    log_score = -sum(math.log(min(1 - eps, max(eps, p))) if y else math.log(1 - min(1 - eps, max(eps, p)))
                     for p, y in pairs) / n
    base = sum(y for _, y in pairs) / n
    brier_base = sum((base - y) ** 2 for _, y in pairs) / n
    buckets, ece = [], 0.0
    for b in range(bins):
        lo, hi = b / bins, (b + 1) / bins
        members = [(p, y) for p, y in pairs if (lo <= p < hi) or (b == bins - 1 and p == 1.0)]
        if not members:
            buckets.append({"lo": lo, "hi": hi, "n": 0, "mean_p": None, "observed": None})
            continue
        mp = sum(p for p, _ in members) / len(members)
        ob = sum(y for _, y in members) / len(members)
        ece += len(members) / n * abs(ob - mp)
        buckets.append({"lo": lo, "hi": hi, "n": len(members), "mean_p": mp, "observed": ob})
    return {"n": n, "status": "OK", "brier": brier, "log_score": log_score, "ece": ece, "base_rate": base,
            "brier_baseline": brier_base, "brier_skill": (1 - brier / brier_base) if brier_base > 0 else None,
            "buckets": buckets}


def stratify(records: Sequence[Mapping[str, Any]], key: Callable[[Mapping[str, Any]], Any], *, value: str,
             min_n: int, reps: int, confidence: float, seed: int, cost_key: Optional[str] = None,
             gross_key: Optional[str] = None) -> Dict[str, Any]:
    groups: Dict[str, List[Mapping[str, Any]]] = {}
    for r in records:
        groups.setdefault(str(key(r)), []).append(r)
    out: Dict[str, Any] = {}
    for k in sorted(groups):
        rows = groups[k]
        vals = [float(r[value]) for r in rows]
        entry: Dict[str, Any] = {"count": len(rows)}
        if len(rows) < min_n:
            entry.update(status=INSUFFICIENT, mean_R=sum(vals) / len(vals))
        else:
            s = expectancy_summary(vals, reps=reps, confidence=confidence, seed=seed, min_n=min_n)
            dd = drawdown_and_tail(vals)
            entry.update(status="OK", mean_R=s["mean_R"], ci_low=s["ci_low"], ci_high=s["ci_high"],
                         p_expectancy_positive=s["p_expectancy_positive"], win_rate=s["win_rate"],
                         max_drawdown_R=dd["max_drawdown_R"], worst_trade_R=dd["worst_trade_R"])
        if cost_key and gross_key:
            gross = sum(abs(float(r[gross_key])) for r in rows)
            cost = sum(float(r[cost_key]) for r in rows)
            entry["cost_share"] = (cost / gross) if gross > 0 else None
        out[k] = entry
    return out


def concentration(records: Sequence[Mapping[str, Any]], key: Callable[[Mapping[str, Any]], Any], *,
                  value: str) -> Dict[str, Any]:
    """How much of the POSITIVE result each segment explains."""
    sums: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    for r in records:
        k = str(key(r))
        sums[k] = sums.get(k, 0.0) + float(r[value])
        counts[k] = counts.get(k, 0) + 1
    total_n = sum(counts.values())
    positive_total = sum(v for v in sums.values() if v > 0)
    net_total = sum(sums.values())
    segments = {k: {"count": counts[k], "count_share": counts[k] / total_n if total_n else None,
                    "net_R_sum": sums[k],
                    "positive_share": (max(0.0, sums[k]) / positive_total) if positive_total > 0 else None,
                    "net_share": (sums[k] / net_total) if net_total > 0 else None}
                for k in sorted(sums)}
    largest = max(segments.items(), key=lambda kv: (kv[1]["positive_share"] or 0.0, kv[0]), default=(None, None))
    return {"segments": segments, "largest_segment": largest[0],
            "largest_positive_share": (largest[1] or {}).get("positive_share") if largest[1] else None,
            "largest_count_share": max((s["count_share"] or 0.0 for s in segments.values()), default=None),
            "total_net_R": net_total}


def _moments(vals: Sequence[float]) -> Tuple[float, float, float, float]:
    n = len(vals)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / n
    sd = math.sqrt(var)
    if sd == 0:
        return mean, 0.0, 0.0, 3.0
    skew = sum(((v - mean) / sd) ** 3 for v in vals) / n
    kurt = sum(((v - mean) / sd) ** 4 for v in vals) / n
    return mean, sd, skew, kurt


def deflated_sharpe(values: Sequence[float], *, n_trials: int, trial_sharpe_variance: float = 0.0,
                    min_n: int = 10) -> Dict[str, Any]:
    """Per-trade Sharpe with the Bailey & Lopez de Prado Deflated Sharpe Ratio
    (non-normality and the number of trials N accounted for). With N == 1 the
    benchmark is 0 and the DSR equals the Probabilistic Sharpe Ratio."""
    vals = [float(v) for v in values]
    n = len(vals)
    if n < min_n:
        return {"n": n, "status": INSUFFICIENT}
    mean, sd, skew, kurt = _moments(vals)
    if sd == 0:
        return {"n": n, "status": INSUFFICIENT, "reason": "ZERO_VARIANCE"}
    sr = mean / sd
    trials = max(1, int(n_trials))
    if trials > 1 and trial_sharpe_variance > 0:
        gamma = 0.5772156649015329
        sr0 = math.sqrt(trial_sharpe_variance) * ((1 - gamma) * _N.inv_cdf(1 - 1 / trials)
                                                  + gamma * _N.inv_cdf(1 - 1 / (trials * math.e)))
    else:
        sr0 = 0.0
    denom = 1 - skew * sr + (kurt - 1) / 4 * sr ** 2
    z = (sr - sr0) * math.sqrt(n - 1) / math.sqrt(denom) if denom > 0 else float("nan")
    return {"n": n, "status": "OK", "sharpe_per_trade": sr, "skew": skew, "kurtosis": kurt, "n_trials": trials,
            "benchmark_sharpe": sr0, "deflated_sharpe_ratio": _N.cdf(z) if not math.isnan(z) else None}


def pbo_cscv(performance: Mapping[str, Sequence[float]], *, partitions: int, min_variants: int = 2,
             min_obs_per_partition: int = 2) -> Dict[str, Any]:
    """Probability of Backtest Overfitting via Combinatorially Symmetric
    Cross-Validation. ``performance[variant]`` is a chronological sequence of
    per-trade (or per-period) results for EACH variant actually compared.
    NOT_APPLICABLE (with a reason) whenever the method is not valid."""
    variants = sorted(performance)
    if len(variants) < min_variants:
        return {"status": "NOT_APPLICABLE", "reason": "FEWER_THAN_TWO_COMPARED_VARIANTS", "n_variants": len(variants)}
    if partitions < 2 or partitions % 2:
        return {"status": "NOT_APPLICABLE", "reason": "CSCV_NEEDS_EVEN_PARTITIONS", "n_variants": len(variants)}
    length = min(len(performance[v]) for v in variants)
    if length < partitions * min_obs_per_partition:
        return {"status": "NOT_APPLICABLE", "reason": "TOO_FEW_OBSERVATIONS_PER_PARTITION",
                "n_variants": len(variants), "observations": length}
    size = length // partitions
    blocks = {v: [list(performance[v][i * size:(i + 1) * size]) for i in range(partitions)] for v in variants}

    def score(v: str, idx: Iterable[int]) -> float:
        xs = [x for i in idx for x in blocks[v][i]]
        return sum(xs) / len(xs) if xs else float("-inf")

    logits, below = [], 0
    combos = list(itertools.combinations(range(partitions), partitions // 2))
    for train in combos:
        test = tuple(i for i in range(partitions) if i not in train)
        best = max(variants, key=lambda v: (score(v, train), v))
        oos = sorted(variants, key=lambda v: (score(v, test), v))
        rank = (oos.index(best) + 1) / (len(variants) + 1)
        logit = math.log(rank / (1 - rank))
        logits.append(logit)
        below += logit <= 0
    return {"status": "OK", "pbo": below / len(combos), "n_variants": len(variants), "partitions": partitions,
            "combinations": len(combos), "selection": "IN_SAMPLE_MEAN_R_ARGMAX",
            "median_logit": median(logits),
            "limitations": "variants are predeclared parameter neighbors; no variant was selected or promoted"}


__all__ = ["expectancy_summary", "drawdown_and_tail", "calibration_summary", "stratify", "concentration",
           "deflated_sharpe", "pbo_cscv", "block_length", "INSUFFICIENT"]
