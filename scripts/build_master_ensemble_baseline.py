"""Master Ensemble baseline measurement (§15.1–§15.5, §15.8, §15.9). Read-only.

The instruction this serves is "measure first, do not tune". So this script
computes the decision funnel, the funnel ratios, the confidence and consensus
distributions, the regime distribution and the component contributions from
canonical ``trading_decisions`` — and it names the dominant bottleneck from the
numbers rather than from intuition.

It changes nothing, and it deliberately reports the sample size next to every
conclusion, because §15.8 forbids removing a component on the strength of a
tiny sample and the same caution applies to every other verdict here.

    python scripts/build_master_ensemble_baseline.py
    python scripts/build_master_ensemble_baseline.py --bot bot_a8117dc719fc --json
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from collections import Counter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND = os.path.join(REPO_ROOT, "backends", "bot-backend")
SHARED = os.path.join(REPO_ROOT, "backends", "shared")

#: A heartbeat is cycle evidence, not an evaluation. Counting it in a funnel
#: makes every ratio meaningless -- it is 98% of the rows.
HEARTBEAT = "NO_NEW_CANDLE"

#: §15.1 canonical funnel order, most upstream first.
FUNNEL_ORDER = (
    "NO_NEW_CANDLE",
    "NO_OPPORTUNITY",
    "REGIME_LOW_VOL_CHOP",
    "REGIME_BLOCKED",
    "SESSION_BLOCKED",
    "EVENT_BLACKOUT",
    "VOLATILITY_SPIKE",
    "INSUFFICIENT_CONSENSUS",
    "CONSENSUS_INSUFFICIENT",
    "ENTRY_CONFIDENCE_BELOW_THRESHOLD",
    "HTF_OPPOSED",
    "HTF_NOT_ALIGNED",
    "RISK_BLOCKED",
    "CORRELATION_BLOCKED",
    "EXECUTION_BLOCKED",
    "DUPLICATE_ENTRY",
    "APPROVED_FOR_EXECUTION",
)


def percentiles(values: list[float]) -> dict:
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    def at(q):
        return ordered[min(len(ordered) - 1, int(len(ordered) * q))]
    return {
        "n": len(ordered),
        "min": ordered[0],
        "p10": at(0.10),
        "p50": at(0.50),
        "p90": at(0.90),
        "max": ordered[-1],
        "mean": statistics.fmean(ordered),
    }


def _safe_json(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _month(iso: str | None) -> str:
    return str(iso or "")[:7] or "unknown"


def _year(iso: str | None) -> str:
    return str(iso or "")[:4] or "unknown"


def _inferred_direction(row: dict) -> str:
    buy = float(row.get("buy_score") or 0.0)
    sell = float(row.get("sell_score") or 0.0)
    if buy > sell:
        return "LONG"
    if sell > buy:
        return "SHORT"
    return "FLAT"


def _gap_bucket(value: float) -> str:
    if value < -0.20:
        return "below_by_more_than_20pct"
    if value < -0.10:
        return "below_by_10_to_20pct"
    if value < -0.05:
        return "below_by_5_to_10pct"
    if value < 0:
        return "below_by_less_than_5pct"
    if value == 0:
        return "at_threshold_or_unresolved"
    return "above_threshold"


def build(db, *, bot_id: str | None, provenance: str | None) -> dict:
    where, args = ["1=1"], []
    if bot_id:
        where.append("bot_instance_id=?")
        args.append(bot_id)
    if provenance:
        where.append("provenance=?")
        args.append(provenance)

    with db.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM trading_decisions WHERE {' AND '.join(where)} "
            f"ORDER BY evaluated_at", tuple(args),
        )]

    if not rows:
        return {"error": "no decisions matched"}

    heartbeats = [r for r in rows if r["primary_reason"] == HEARTBEAT]
    evaluations = [r for r in rows if r["primary_reason"] != HEARTBEAT]

    report: dict = {
        "window": {"from": rows[0]["evaluated_at"], "to": rows[-1]["evaluated_at"]},
        "bots": sorted({r["bot_instance_id"] for r in rows}),
        "provenance": sorted({str(r["provenance"]) for r in rows}),
        "symbols": dict(Counter(r["symbol"] for r in evaluations)),
        "totals": {
            "decision_rows": len(rows),
            "heartbeats": len(heartbeats),
            "real_evaluations": len(evaluations),
        },
    }

    # §15.1 funnel
    counts = Counter(str(r["primary_reason"]) for r in rows)
    ordered = {k: counts[k] for k in FUNNEL_ORDER if counts.get(k)}
    ordered.update({k: v for k, v in counts.items() if k not in FUNNEL_ORDER})
    report["funnel"] = ordered

    # §15.2 ratios, over real evaluations only
    n = len(evaluations) or 1
    opportunities = [r for r in evaluations if (r["raw_confidence"] or 0) > 0]
    approved = [r for r in evaluations if r["final_action"] == "APPROVED"]
    risk_seen = [r for r in evaluations if r["risk_result"] is not None]
    feasible = [r for r in evaluations
                if r["execution_feasibility_result"] not in (None, "FAIL")]
    attempts = [r for r in evaluations if r["execution_attempt_id"]]
    fills = [r for r in evaluations if r["position_id"]]
    report["ratios"] = {
        "closed_candles_evaluated": len(evaluations),
        "produced_an_opportunity": len(opportunities),
        "quality_approved": len(approved),
        "reached_risk": len(risk_seen),
        "execution_feasible": len(feasible),
        "execution_attempts": len(attempts),
        "fills": len(fills),
        "opportunity_rate": len(opportunities) / n,
        "approval_rate": len(approved) / n,
    }
    report["reason_counts"] = reason_counts(rows, evaluations)
    report["transition_rates"] = transition_rates(report["ratios"])

    # §15.3 / §15.4 distributions
    def col(key):
        return [float(r[key]) for r in evaluations if r[key] is not None]

    report["confidence"] = {
        "raw_confidence": percentiles(col("raw_confidence")),
        "effective_entry_threshold": percentiles(col("effective_entry_threshold")),
        "confidence_minus_threshold": percentiles([
            float(r["raw_confidence"]) - float(r["effective_entry_threshold"])
            for r in evaluations
            if r["raw_confidence"] is not None
            and r["effective_entry_threshold"] is not None
        ]),
    }
    report["threshold_gap_buckets"] = dict(Counter(
        _gap_bucket(float(r["raw_confidence"]) - float(r["effective_entry_threshold"]))
        for r in evaluations
        if r["raw_confidence"] is not None
        and r["effective_entry_threshold"] is not None
    ))
    report["consensus"] = {
        "buy_score": percentiles(col("buy_score")),
        "sell_score": percentiles(col("sell_score")),
        "consensus_observed": percentiles(col("consensus_observed")),
    }
    report["confidence_distributions_by_dimension"] = confidence_by_dimension(evaluations)

    # §15.5 regimes
    report["regimes"] = dict(Counter(
        str(r["regime"]) for r in evaluations if r["regime"] is not None
    ))
    report["regime_profitability_placeholder"] = (
        "populated from closed positions when fills exist"
    )

    # §15.8 component contribution
    active, supporting, opposing = Counter(), Counter(), Counter()
    for r in evaluations:
        for name in _safe_json(r["active_strategies_json"], []):
            active[name] += 1
        for name in _safe_json(r["supporting_strategies_json"], []):
            supporting[name] += 1
        for name in _safe_json(r["opposing_strategies_json"], []):
            opposing[name] += 1
    report["components"] = {
        "active": dict(active),
        "supporting": dict(supporting),
        "opposing": dict(opposing),
        "never_active": sorted(set(_ALL_COMPONENTS) - set(active)),
        "never_supported": sorted(set(active) - set(supporting)),
        "support_count_distribution": support_distribution(evaluations),
    }

    report["profitability"] = profitability(db, bot_id=bot_id, provenance=provenance)
    report["diagnosis"] = diagnose(report, evaluations)
    report["phase15_classification"] = classify_phase15(report)
    report["benchmark_preservation"] = historical_benchmark()
    return report


_ALL_COMPONENTS = (
    "supertrend", "vwap_reversion", "trend_pullback", "squeeze_breakout",
    "sma_cross", "donchian_breakout", "bollinger_reversion",
)


def reason_counts(all_rows: list[dict], evaluations: list[dict]) -> dict:
    """§15.1 complete no-trade/block/approved/attempt/fill reason counts."""
    no_trade = Counter()
    block = Counter()
    approved = Counter()
    attempts = Counter()
    fills = Counter()
    for row in evaluations:
        reason = str(row.get("primary_reason") or "UNKNOWN")
        action = str(row.get("final_action") or "")
        if action == "APPROVED" or reason == "APPROVED_FOR_EXECUTION":
            approved[reason] += 1
        elif reason.startswith(("RISK_", "EXECUTION_", "REGIME_", "SESSION_", "EVENT_", "HTF_")):
            block[reason] += 1
        else:
            no_trade[reason] += 1
        if row.get("execution_attempt_id"):
            attempts[reason] += 1
        if row.get("position_id"):
            fills[reason] += 1
    return {
        "all_rows": dict(Counter(str(r.get("primary_reason") or "UNKNOWN") for r in all_rows)),
        "no_trade": dict(no_trade),
        "block": dict(block),
        "approved": dict(approved),
        "attempt": dict(attempts),
        "fill": dict(fills),
    }


def transition_rates(ratios: dict) -> dict:
    """§15.2 transition rates between funnel stages."""
    def rate(num, den):
        return (num / den) if den else 0.0

    evals = ratios.get("closed_candles_evaluated", 0)
    opps = ratios.get("produced_an_opportunity", 0)
    approved = ratios.get("quality_approved", 0)
    risk = ratios.get("reached_risk", 0)
    feasible = ratios.get("execution_feasible", 0)
    attempts = ratios.get("execution_attempts", 0)
    fills = ratios.get("fills", 0)
    return {
        "evaluation_to_opportunity": rate(opps, evals),
        "opportunity_to_quality_approved": rate(approved, opps),
        "approved_to_risk_seen": rate(risk, approved),
        "risk_seen_to_execution_feasible": rate(feasible, risk),
        "feasible_to_attempt": rate(attempts, feasible),
        "attempt_to_fill": rate(fills, attempts),
    }


def confidence_by_dimension(rows: list[dict]) -> dict:
    """§15.3 confidence/threshold/support distributions by audit dimensions."""
    dimensions = {
        "symbol": lambda r: str(r.get("symbol") or "unknown"),
        "direction": _inferred_direction,
        "regime": lambda r: str(r.get("regime") or "unknown"),
        "session": lambda r: _session_bucket(r.get("evaluated_at")),
        "month": lambda r: _month(r.get("evaluated_at")),
        "year": lambda r: _year(r.get("evaluated_at")),
        "decision_outcome": lambda r: (
            "approved" if str(r.get("final_action") or "") == "APPROVED"
            else "rejected"
        ),
    }
    out: dict[str, dict] = {}
    for name, key_fn in dimensions.items():
        buckets: dict[str, list[dict]] = {}
        for row in rows:
            buckets.setdefault(key_fn(row), []).append(row)
        out[name] = {
            bucket: {
                "rows": len(group),
                "raw_confidence": percentiles([
                    float(r["raw_confidence"]) for r in group
                    if r.get("raw_confidence") is not None
                ]),
                "effective_entry_threshold": percentiles([
                    float(r["effective_entry_threshold"]) for r in group
                    if r.get("effective_entry_threshold") is not None
                ]),
                "support_count": percentiles([
                    float(len(_safe_json(r.get("supporting_strategies_json"), [])))
                    for r in group
                ]),
            }
            for bucket, group in sorted(buckets.items())
        }
    return out


def support_distribution(rows: list[dict]) -> dict:
    return dict(Counter(
        len(_safe_json(row.get("supporting_strategies_json"), []))
        for row in rows
    ))


def diagnose(report: dict, evaluations: list[dict]) -> dict:
    """§15.9 — name the dominant bottleneck from the numbers, not from a hunch."""
    n = len(evaluations)
    findings: list[str] = []

    confidence = report["confidence"]["raw_confidence"]
    threshold = report["confidence"]["effective_entry_threshold"]
    no_direction = sum(1 for r in evaluations if not (r["raw_confidence"] or 0))

    if n and no_direction / n >= 0.5:
        findings.append(
            f"NO_DIRECTIONAL_CANDIDATE is dominant: {no_direction}/{n} "
            f"({no_direction / n:.0%}) of evaluations produced no opportunity at "
            f"all, so the entry threshold was never consulted on them."
        )
    if confidence.get("n") and threshold.get("n"):
        if confidence["p50"] == 0:
            findings.append(
                "Median raw confidence is exactly 0.0 — on a typical candle the "
                "ensemble has no directional conviction whatsoever."
            )
        if confidence["max"] < threshold.get("p50", 0):
            findings.append(
                f"Even the best observed confidence ({confidence['max']:.4f}) is "
                f"below the median threshold ({threshold['p50']:.4f})."
            )

    never_supported = report["components"]["never_supported"]
    if never_supported:
        findings.append(
            f"Components active but never supporting a direction: "
            f"{', '.join(never_supported)}."
        )
    never_active = report["components"]["never_active"]
    if never_active:
        findings.append(
            f"Components never active in this window (regime-disabled): "
            f"{', '.join(never_active)}."
        )

    approvals = report["ratios"]["quality_approved"]
    if approvals == 0:
        findings.append(
            f"Zero approvals in {n} evaluations: nothing reached the risk layer, "
            f"so risk, sizing, feasibility and execution are entirely unmeasured."
        )

    return {
        "sample_size": n,
        "sample_adequacy": (
            "ADEQUATE" if n >= 500 else
            "THIN" if n >= 100 else
            "TOO_SMALL_FOR_COMPONENT_DECISIONS"
        ),
        "findings": findings or ["No dominant bottleneck identified."],
    }


# ── §15.6 / §15.7 Profitability, after costs ────────────────────────────────


def _session_bucket(iso: str | None) -> str:
    """Crude UTC session label. Named as crude so nobody over-reads it."""
    if not iso:
        return "unknown"
    try:
        hour = int(str(iso)[11:13])
    except Exception:
        return "unknown"
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 16:
        return "europe"
    return "americas"


def _drawdown(pnls: list[float]) -> dict:
    """Peak-to-trough on the realized equity curve, in currency and percent."""
    equity = 0.0
    peak = 0.0
    worst = 0.0
    worst_pct = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drop = peak - equity
        if drop > worst:
            worst = drop
            worst_pct = drop / peak if peak > 0 else 0.0
    return {"max_drawdown": worst, "max_drawdown_pct": worst_pct}


def profitability(db, *, bot_id: str | None, provenance: str | None) -> dict:
    """Realized results for closed positions, net of the fees actually charged.

    Gross numbers are not reported. A strategy that is profitable before costs
    and unprofitable after them is unprofitable, and showing the gross figure
    invites the wrong conclusion.
    """
    where, args = ["status='CLOSED'"], []
    if bot_id:
        where.append("bot_instance_id=?")
        args.append(bot_id)
    if provenance:
        where.append("provenance=?")
        args.append(provenance)

    with db.connect() as conn:
        positions = [dict(r) for r in conn.execute(
            f"SELECT * FROM positions WHERE {' AND '.join(where)} ORDER BY opened_at",
            tuple(args),
        )]
        decisions = {
            r["decision_id"]: dict(r) for r in conn.execute(
                "SELECT decision_id, regime, symbol, raw_confidence, "
                "consensus_observed, stop_price, supporting_strategies_json "
                "FROM trading_decisions"
            )
        }

    if not positions:
        return {"closed_trades": 0,
                "note": "no closed positions; profitability is not measurable"}

    trades = []
    for row in positions:
        gross = float(row.get("realized_pnl") or 0.0)
        fees = float(row.get("fees") or 0.0)
        net = gross - fees
        decision = decisions.get(row.get("decision_id")) or {}
        entry = float(row.get("entry_price") or 0.0)
        qty = float(row.get("original_qty") or 0.0)
        stop = float(decision.get("stop_price") or 0.0)
        risk = abs(entry - stop) * qty if stop > 0 and entry > 0 else None
        trades.append({
            "symbol": row.get("symbol"),
            "side": str(row.get("side") or "").upper(),
            "gross": gross, "fees": fees, "net": net,
            "regime": decision.get("regime") or "unknown",
            "month": str(row.get("opened_at") or "")[:7],
            "year": str(row.get("opened_at") or "")[:4],
            "session": _session_bucket(row.get("opened_at")),
            "r_multiple": (net / risk) if risk else None,
            "notional": entry * qty,
            "components": _safe_json(
                decision.get("supporting_strategies_json")
                if decision else None,
                [],
            ),
        })

    nets = [t["net"] for t in trades]
    wins = [n for n in nets if n > 0]
    losses = [n for n in nets if n < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    r_values = [t["r_multiple"] for t in trades if t["r_multiple"] is not None]

    summary = {
        "closed_trades": len(trades),
        "gross_pnl": sum(t["gross"] for t in trades),
        "total_fees": sum(t["fees"] for t in trades),
        "net_pnl": sum(nets),
        "expectancy_per_trade": sum(nets) / len(nets),
        "win_rate": len(wins) / len(nets),
        "wins": len(wins), "losses": len(losses),
        "average_win": (gross_profit / len(wins)) if wins else 0.0,
        "average_loss": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "average_r_multiple": (sum(r_values) / len(r_values)) if r_values else None,
        **_drawdown(nets),
    }

    def segment(key: str) -> dict:
        out: dict[str, dict] = {}
        for trade in trades:
            bucket = out.setdefault(str(trade[key]), {"trades": 0, "net_pnl": 0.0, "wins": 0})
            bucket["trades"] += 1
            bucket["net_pnl"] += trade["net"]
            bucket["wins"] += 1 if trade["net"] > 0 else 0
        for bucket in out.values():
            bucket["win_rate"] = bucket["wins"] / bucket["trades"]
            bucket["expectancy"] = bucket["net_pnl"] / bucket["trades"]
        return out

    return {
        **summary,
        "by_regime": segment("regime"),
        "by_symbol": segment("symbol"),
        "by_side": segment("side"),
        "by_month": segment("month"),
        "by_year": segment("year"),
        "by_session": segment("session"),
        "by_component": profitability_by_component(trades),
    }


def profitability_by_component(trades: list[dict]) -> dict:
    out: dict[str, dict] = {}
    for trade in trades:
        components = trade.get("components") or ["NO_SUPPORTING_COMPONENT"]
        for component in components:
            bucket = out.setdefault(str(component), {"trades": 0, "net_pnl": 0.0, "wins": 0})
            bucket["trades"] += 1
            bucket["net_pnl"] += trade["net"]
            bucket["wins"] += 1 if trade["net"] > 0 else 0
    for bucket in out.values():
        bucket["win_rate"] = bucket["wins"] / bucket["trades"]
        bucket["expectancy"] = bucket["net_pnl"] / bucket["trades"]
    return out


def classify_phase15(report: dict) -> dict:
    """§15 final gate: choose exactly one current-state classification."""
    evaluations = int(report.get("totals", {}).get("real_evaluations") or 0)
    closed = int(report.get("profitability", {}).get("closed_trades") or 0)
    approvals = int(report.get("ratios", {}).get("quality_approved") or 0)
    if evaluations < 500 or closed < 30:
        label = "E_INCONCLUSIVE"
        reason = (
            "sample lacks enough evaluations and/or closed trades for a "
            "primary-strategy retention or replacement decision"
        )
    elif approvals == 0:
        label = "D_REPLACE_AS_PRIMARY_OPPORTUNITY_PRODUCER"
        reason = "adequate sample produced no approved entries"
    else:
        profit = report.get("profitability", {})
        expectancy = float(profit.get("expectancy_per_trade") or 0.0)
        pf = profit.get("profit_factor")
        if expectancy > 0 and (pf is None or float(pf) >= 1.3):
            label = "A_RETAIN_AS_PRIMARY"
            reason = "closed-trade expectancy and profit factor pass current gates"
        elif expectancy > 0:
            label = "B_RETAIN_BUT_TUNE_LATER"
            reason = "expectancy is positive but profit factor is below the target"
        else:
            label = "C_RETAIN_AS_EXPERT_LAYER"
            reason = "entry evidence exists but closed-trade economics do not justify primary status"
    return {"classification": label, "reason": reason}


def historical_benchmark() -> list[dict]:
    """§52 before/after benchmark that must stay visible in Phase 15/16."""
    return [
        {
            "period": "MAY_2026",
            "historical_issue": "STOP_TOO_WIDE historical unit/path defect",
            "current_status": "not part of current Master Ensemble baseline path",
        },
        {
            "period": "JULY_2026_EXECUTION_DEFECT",
            "historical_issue": "PAPER_ONLY without actual simulated fills",
            "current_status": "Phase 12/13 evidence path records attempts, fills and positions when approvals occur",
        },
        {
            "period": "JULY_2026_NATURAL_BEHAVIOR",
            "historical_issue": "strategy_no_signal/HOLD / opportunity scarcity",
            "current_status": "still the dominant observed bottleneck when opportunity rate is low",
        },
        {
            "period": "SEPTEMBER_2026",
            "historical_issue": "multiple confidence authorities, same-candle repeated evaluation, generic HOLD evidence, configuration conflict, lifecycle defects",
            "current_status": "threshold authority and lifecycle are tested separately; baseline still measures zero downstream exercise when approvals are absent",
        },
    ]


def render(report: dict) -> str:
    if "error" in report:
        return report["error"]
    out: list[str] = []
    t = report["totals"]
    out.append(f"window            {report['window']['from']} -> {report['window']['to']}")
    out.append(f"bots              {', '.join(report['bots'])}")
    out.append(f"provenance        {', '.join(report['provenance'])}")
    out.append(f"symbols           {report['symbols']}")
    out.append("")
    out.append(f"decision rows     {t['decision_rows']:,}")
    out.append(f"  heartbeats      {t['heartbeats']:,}")
    out.append(f"  real evals      {t['real_evaluations']:,}")
    out.append("")
    out.append("DECISION FUNNEL (§15.1)")
    for reason, count in report["funnel"].items():
        out.append(f"  {reason:<36} {count:>7,}")
    out.append("")
    out.append("FUNNEL RATIOS (§15.2)")
    for key, value in report["ratios"].items():
        out.append(f"  {key:<30} {value:.4f}" if isinstance(value, float)
                   else f"  {key:<30} {value:,}")
    out.append("")
    out.append("TRANSITION RATES (§15.2)")
    for key, value in report.get("transition_rates", {}).items():
        out.append(f"  {key:<36} {value:.4f}")
    out.append("")
    out.append("REASON COUNTS (§15.1)")
    for group, counts in report.get("reason_counts", {}).items():
        out.append(f"  {group}")
        for reason, count in sorted((counts or {}).items()):
            out.append(f"    {reason:<34} {count:>7,}")
    out.append("")
    for section, title in (("confidence", "CONFIDENCE (§15.3)"),
                           ("consensus", "CONSENSUS (§15.4)")):
        out.append(title)
        for key, stats in report[section].items():
            if not stats.get("n"):
                out.append(f"  {key:<30} (no data)")
                continue
            out.append(
                f"  {key:<30} n={stats['n']:<5} min={stats['min']:.4f} "
                f"p50={stats['p50']:.4f} p90={stats['p90']:.4f} "
                f"max={stats['max']:.4f} mean={stats['mean']:.4f}"
            )
        out.append("")
    out.append(f"THRESHOLD GAP BUCKETS (§15.3) {report.get('threshold_gap_buckets', {})}")
    out.append("")
    out.append(f"REGIMES (§15.5)     {report['regimes']}")
    out.append("")
    out.append("COMPONENTS (§15.8)")
    for key, value in report["components"].items():
        out.append(f"  {key:<18} {value}")
    out.append("")
    profit = report.get("profitability") or {}
    out.append("PROFITABILITY AFTER COSTS (\u00a715.6)")
    if not profit.get("closed_trades"):
        out.append(f"  {profit.get('note', 'no closed positions')}")
    else:
        for key in ("closed_trades", "net_pnl", "gross_pnl", "total_fees",
                    "expectancy_per_trade", "win_rate", "profit_factor",
                    "average_win", "average_loss", "average_r_multiple",
                    "max_drawdown", "max_drawdown_pct"):
            value = profit.get(key)
            if isinstance(value, float):
                out.append(f"  {key:<24} {value:.6g}")
            else:
                out.append(f"  {key:<24} {value}")
        for segment in ("by_regime", "by_symbol", "by_side", "by_month",
                        "by_year", "by_session", "by_component"):
            out.append(f"  {segment} (\u00a715.7)")
            for name, stats in sorted((profit.get(segment) or {}).items()):
                out.append(
                    f"    {name:<18} trades={stats['trades']:<4} "
                    f"net={stats['net_pnl']:.6g} win_rate={stats['win_rate']:.2%}"
                )
    out.append("")
    diagnosis = report["diagnosis"]
    out.append(f"DIAGNOSIS (§15.9)   sample={diagnosis['sample_size']} "
               f"({diagnosis['sample_adequacy']})")
    for finding in diagnosis["findings"]:
        out.append(f"  * {finding}")
    phase = report.get("phase15_classification") or {}
    out.append("")
    out.append("PHASE 15 CLASSIFICATION")
    out.append(f"  {phase.get('classification', 'UNKNOWN')}: {phase.get('reason', '')}")
    return "\n".join(out)


def markdown_report(report: dict) -> str:
    """Durable Phase 15 report; intentionally generated from JSON fields."""
    lines = [
        "# CosmicForge - Phase 15 Master Ensemble baseline",
        "",
        "Measured evidence only. This report is generated from the accompanying "
        "`phase15_master_ensemble_baseline_results.json` artifact.",
        "",
        "## Scope",
        "",
        f"- Window: `{report.get('window', {}).get('from')}` -> `{report.get('window', {}).get('to')}`",
        f"- Bots: `{', '.join(report.get('bots', []))}`",
        f"- Provenance: `{', '.join(report.get('provenance', []))}`",
        f"- Real evaluations: `{report.get('totals', {}).get('real_evaluations', 0)}`",
        f"- Decision rows: `{report.get('totals', {}).get('decision_rows', 0)}`",
        "",
        "## Decision Funnel",
        "",
        "| Reason | Count |",
        "| --- | ---: |",
    ]
    for reason, count in (report.get("funnel") or {}).items():
        lines.append(f"| `{reason}` | {count} |")
    lines.extend([
        "",
        "## Transition Rates",
        "",
        "| Transition | Rate |",
        "| --- | ---: |",
    ])
    for name, value in (report.get("transition_rates") or {}).items():
        lines.append(f"| `{name}` | {float(value):.4f} |")
    lines.extend([
        "",
        "## Confidence And Thresholds",
        "",
        "```json",
        json.dumps({
            "confidence": report.get("confidence"),
            "threshold_gap_buckets": report.get("threshold_gap_buckets"),
            "consensus": report.get("consensus"),
        }, indent=2, sort_keys=True),
        "```",
        "",
        "## Regime And Component Evidence",
        "",
        "```json",
        json.dumps({
            "regimes": report.get("regimes"),
            "components": report.get("components"),
        }, indent=2, sort_keys=True),
        "```",
        "",
        "## Profitability After Costs",
        "",
        "```json",
        json.dumps(report.get("profitability"), indent=2, sort_keys=True),
        "```",
        "",
        "## Diagnosis",
        "",
    ])
    for finding in (report.get("diagnosis") or {}).get("findings", []):
        lines.append(f"- {finding}")
    phase = report.get("phase15_classification") or {}
    lines.extend([
        "",
        "## Phase 15 Classification",
        "",
        f"`{phase.get('classification', 'UNKNOWN')}` - {phase.get('reason', '')}",
        "",
        "## Historical Benchmark",
        "",
        "| Period | Historical Issue | Current Status |",
        "| --- | --- | --- |",
    ])
    for row in report.get("benchmark_preservation") or []:
        lines.append(
            f"| `{row['period']}` | {row['historical_issue']} | {row['current_status']} |"
        )
    lines.extend([
        "",
        "## Phase 16 Implication",
        "",
        "This artifact does not start or complete Phase 16. Phase 16 still requires "
        "fresh forward-paper identity, at least three consecutive real forward "
        "weeks, and at least 60 correctly closed campaign trades.",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=None)
    parser.add_argument("--bot", default=None)
    parser.add_argument("--provenance", default=None,
                        help="restrict to one provenance, e.g. PAPER_FORWARD")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out-json", default=None, help="write machine-readable artifact")
    parser.add_argument("--out-md", default=None, help="write Markdown report artifact")
    args = parser.parse_args()
    db_path = None
    if args.db:
        db_path = args.db if os.path.isabs(args.db) else os.path.join(REPO_ROOT, args.db)

    for path in (BACKEND, SHARED):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.chdir(BACKEND)
    env = os.path.join(BACKEND, ".env")
    if os.path.exists(env):
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env, override=True)

    from shared_lib.persistence.db import DB

    report = build(DB(db_path) if db_path else DB(),
                   bot_id=args.bot, provenance=args.provenance)
    if args.out_json:
        out_json = args.out_json if os.path.isabs(args.out_json) else os.path.join(REPO_ROOT, args.out_json)
        os.makedirs(os.path.dirname(out_json), exist_ok=True)
        with open(out_json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
    if args.out_md:
        out_md = args.out_md if os.path.isabs(args.out_md) else os.path.join(REPO_ROOT, args.out_md)
        os.makedirs(os.path.dirname(out_md), exist_ok=True)
        with open(out_md, "w", encoding="utf-8") as handle:
            handle.write(markdown_report(report))
    print(json.dumps(report, indent=2, default=str) if args.json else render(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
